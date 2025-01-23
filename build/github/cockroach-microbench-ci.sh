#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

# Directories and file names
output_dir="./artifacts/microbench"
cleaned_current_dir="$output_dir/$GITHUB_REF_NAME/$GITHUB_SHA"
cleaned_base_dir="$output_dir/$GITHUB_BASE_REF/$BASE_SHA"

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
bazel build --config crosslinux \
  $(./build/github/engflow-args.sh) \
  --jobs 100  \
  //pkg/sql/tests:tests_test

curr_benchmark_binary="_bazel/bin/pkg/sql/tests/tests_test_/tests_test"

mkdir -p "$output_dir"
mkdir -p "$cleaned_current_dir"
touch "$cleaned_current_dir/output.log" "$cleaned_current_dir/err.log"

# Run benchmarks and clean output
# running count=4 here because that's the minimum required for a comparison

max=20
for _ in $(seq 1 $max)
do
  $curr_benchmark_binary -test.run - -test.bench "^BenchmarkSysbench/SQL/3node/oltp_write_only$" \
    -test.benchmem \
    -test.cpu 4 \
    -test.benchtime 1000x \
    -test.outputdir "$cleaned_current_dir" \
    -test.memprofile "mem.prof" \
    -test.cpuprofile "cpu.prof" \
    -test.mutexprofile "mutex.prof" >> "$cleaned_current_dir/output.log" 2>> "$cleaned_current_dir/err.log"
done

cat "$cleaned_current_dir/output.log"

#go install golang.org/x/perf/cmd/benchstat

# Push artifact if this is a base merge and skip comparison
#if $PUSH_STEP; then
#gcloud storage cp "$cleaned_current_dir/" "$storage_bucket_url/$GITHUB_REF_NAME/$GITHUB_SHA.log"
#  echo "Skipping comparison since this is a push step into the target branch"
#  exit $success_exit_status
#fi
#
## Compare benchmarks
#if ! gcloud storage cp "$storage_bucket_url/$GITHUB_BASE_REF/$BASE_SHA.log" "$cleaned_base_dir/$benchmark_file_name"; then
#  echo "Couldn't download base bench file, exiting."
#  exit $success_exit_status
#fi
#
#if ! $roachprod_microbench_dir/roachprod-microbench compare "$cleaned_current_dir" "$cleaned_base_dir" --threshold="$threshold"; then
#  echo "There is an error during comparison. If it's a perf regression, please try to fix it. This won't block your change for merging currently."
#  exit $error_exit_status
#fi

rm -rf "$output_dir"
exit $success_exit_status
