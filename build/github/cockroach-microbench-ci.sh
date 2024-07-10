#!/bin/bash

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
threshold=$COMPARE_THRESHOLD
exit_status=0

# Configure Bazel and dev tooling
cat <<EOF > ./.bazelrc.user
build --config nolintonbuild
build --remote_cache=http://127.0.0.1:9867
test --test_tmpdir=/tmp/cockroach
test --config crosslinux
EOF

# Exit early if SKIP_COMPARISON is true and not a push step
if $SKIP_COMPARISON && ! $PUSH_STEP; then
  echo "Exiting since skip comparison is enabled and this is not a push step."
  exit $exit_status
fi

./dev doctor
./dev build roachprod-microbench

# Run benchmarks and clean output
mkdir -p "$output_dir"

# running 4 here because that's the minumum required for a comparison
./dev bench pkg/sql/tests '--filter=^BenchmarkKV$' --count 4 --verbose --bench-mem > "$log_output_file_path"
./bin/roachprod-microbench clean "$log_output_file_path" "$cleaned_current_dir/$benchmark_file_name"

# Push artifact if this is a base merge and skip comparison
if $PUSH_STEP; then
  gcloud storage cp "$cleaned_current_dir/$benchmark_file_name" "$storage_bucket_url/$GITHUB_HEAD_REF/$GITHUB_SHA.log"
  echo "Skipping comparison since this is a base merge."
  exit $exit_status
fi

# Compare benchmarks
if ! gcloud storage cp "$storage_bucket_url/$GITHUB_BASE_REF/$BASE_SHA.log" "$cleaned_base_dir/$benchmark_file_name"; then
  echo "Couldn't download base bench file, exiting."
  exit $exit_status
fi

if ! ./bin/roachprod-microbench compare "$cleaned_current_dir" "$cleaned_base_dir" --threshold "$threshold"; then
  echo "There is an error during comparison. If it's a perf regression, please fix it. If a regression is expected, add [PERF_REGRESSION_EXPECTED] in the PR head title."
  exit 1
fi

rm -rf $output_dir
exit $exit_status
