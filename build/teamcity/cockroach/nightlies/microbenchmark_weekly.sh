#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# This script runs microbenchmarks across a roachprod cluster.
# Parameters:
#   GCS_COMPARE_BINARIES: GCS path to a test binaries archive to compare against.
#   GCE_NODE_COUNT: number of nodes to use in the roachprod cluster (default: 12)
#   CLUSTER_LIFETIME: lifetime of the roachprod cluster (default: 24h)
#   GCE_MACHINE_TYPE: machine type to use in the roachprod cluster (default: n1-standard-8)
#   GCE_ZONE: zone to use in the roachprod cluster (default: us-east4-c)
#   BENCH_PACKAGE: package to build and run benchmarks against (default: ./pkg/...)
#   BENCH_ITERATIONS: number of iterations to run each microbenchmark (default: 10)
#   BENCH_SHELL: command to run before each iteration (default: export COCKROACH_RANDOM_SEED=1)
#   SHEET_DESCRIPTION: Adds a description to the name of the published spreadsheets (e.g., "22.2 -> 22.1")
#   BENCH_TIMEOUT: timeout for each microbenchmark on a function level (default: 20m)
#   BENCH_EXCLUDE: comma-separated list of benchmarks to exclude (default: none)
#   BENCH_IGNORE_PACKAGES: comma-separated list of packages to exclude completely from listing and execution (default: none)
#   TEST_ARGS: additional arguments to pass to the test binary (default: none)
#   ROACHPROD_CREATE_ARGS: additional arguments to pass to `roachprod create` (default: none)
#   MICROBENCH_SLACK_TOKEN: token to use to post to slack (default: none)

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"
output_dir="./artifacts/microbench"
exit_status=0

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
generate_ssh_key
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
export ROACHPROD_USER=teamcity
export ROACHPROD_CLUSTER=teamcity-microbench-${TC_BUILD_ID}

# Configure Bazel and dev tooling
bazelOpts=(
"build --config nolintonbuild"
"build --remote_cache=http://127.0.0.1:9867"
"test --test_tmpdir=/tmp/cockroach"
)
printf "%s\n" "${bazelOpts[@]}" > ./.bazelrc.user

./dev doctor

# Set docker args for bazel support docker
teamcity_alternates="/home/agent/system/git"
docker_args="--volume=${teamcity_alternates}:${teamcity_alternates}:ro"

# Build required tooling
./dev build roachprod
./dev build roachprod-microbench

# Build libgeos and move to lib directory
./dev build libgeos --cross="linux" --docker-args="$docker_args"
mkdir -p ./bin/lib
mv ./artifacts/libgeos* ./bin/lib/

# Copy comparison binaries to bin directory if specified
if [[ -n "${GCS_COMPARE_BINARIES}" ]] ; then
  bench_compare_binaries="./bin/compare_test_binaries.tar.gz"
  gsutil -q cp "$GCS_COMPARE_BINARIES" "$bench_compare_binaries"
fi

# Build test binaries
./dev test-binaries "$BENCH_PACKAGE" --docker-args="$docker_args"

# Create roachprod cluster
./bin/roachprod create "$ROACHPROD_CLUSTER" -n "$GCE_NODE_COUNT" $ROACHPROD_CREATE_ARGS \
  --lifetime "$CLUSTER_LIFETIME" \
  --clouds gce \
  --gce-machine-type "$GCE_MACHINE_TYPE" \
  --gce-zones="$GCE_ZONE" \
  --os-volume-size=128


# Execute microbenchmarks
./bin/roachprod-microbench run "$ROACHPROD_CLUSTER" \
  --output-dir="$output_dir" \
  --iterations "$BENCH_ITERATIONS" \
  --lib-dir=./bin/lib \
  --shell="$BENCH_SHELL" \
  ${bench_compare_binaries:+--compare-binaries="$bench_compare_binaries"} \
  ${BENCH_TIMEOUT:+--timeout="$BENCH_TIMEOUT"} \
  ${BENCH_EXCLUDE:+--exclude="$BENCH_EXCLUDE"} \
  ${BENCH_IGNORE_PACKAGES:+--ignore-package="$BENCH_IGNORE_PACKAGES"} \
  --quiet \
  -- "$TEST_ARGS" \
  || exit_status=$?

# Generate sheets if comparing
if [[ -n "${GCS_COMPARE_BINARIES}" ]]; then
  if [ -d "$output_dir/0" ] && [ "$(ls -A "$output_dir/0")" ] \
  && [ -d "$output_dir/1" ] && [ "$(ls -A "$output_dir/1")" ]; then
    ./bin/roachprod-microbench compare "$output_dir/0" "$output_dir/1" \
      ${MICROBENCH_SLACK_TOKEN:+--slack-token="$MICROBENCH_SLACK_TOKEN"} \
      --sheet-desc="$SHEET_DESCRIPTION" 2>&1 | tee "$output_dir/sheets.txt"
  else
    echo "No microbenchmarks were run. Skipping comparison."
  fi
fi

# Exit with the code from roachprod-microbench
exit $exit_status
