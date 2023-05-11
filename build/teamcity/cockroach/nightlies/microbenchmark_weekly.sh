#!/usr/bin/env bash
#
# This script runs microbenchmarks across a roachprod cluster.
# Parameters:
#   GCS_COMPARE_BINARIES: GCS path to a test binaries archive to compare against,
#   this will be automatically copied to BENCH_COMPARE_BINARIES.
#   BENCH_COMPARE_BINARIES: path to a to a test binaries archive to compare this run against (default: ./bin)
#   GCE_NODE_COUNT: number of nodes to use in the roachprod cluster (default: 12)
#   CLUSTER_LIFETIME: lifetime of the roachprod cluster (default: 24h)
#   GCE_MACHINE_TYPE: machine type to use in the roachprod cluster (default: n1-standard-8)
#   GCE_ZONE: zone to use in the roachprod cluster (default: us-east4-c)
#   BENCH_PACKAGE: package to build and run benchmarks against (default: ./pkg/...)
#   BENCH_ITERATIONS: number of iterations to run each microbenchmark (default: 10)
#   BENCH_SHELL: command to run before each iteration (default: export COCKROACH_RANDOM_SEED=1)
#   SHEET_DESCRIPTION: Adds a description to the name of the published spreadsheets (e.g., "22.2 -> 22.1")
#   BENCH_TIMEOUT: timeout for each microbenchmark on a function level (default: 20m)
#   BENCH_PUBLISH_DIR: directory to publish results to (default: gs://cockroach-microbench)
#   BENCH_COMPARE_DIR: directory to compare results against (default: none)
#   BENCH_EXCLUDE: comma-separated list of benchmarks to exclude (default: none)
#   TEST_ARGS: additional arguments to pass to the test binary (default: none)

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
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
  gsutil cp "$GCS_COMPARE_BINARIES" "$BENCH_COMPARE_BINARIES"
fi

# Build test binaries
./dev test-binaries "$BENCH_PACKAGE" --docker-args="$docker_args"

# Create roachprod cluster
./bin/roachprod create "$ROACHPROD_CLUSTER" -n "$GCE_NODE_COUNT" \
  --lifetime "$CLUSTER_LIFETIME" \
  --clouds gce \
  --gce-machine-type "$GCE_MACHINE_TYPE" \
  --gce-zones="$GCE_ZONE" \
  --os-volume-size=128

# Execute microbenchmarks
./bin/roachprod-microbench ./artifacts/microbench \
  -cluster "$ROACHPROD_CLUSTER" \
  -iterations "$BENCH_ITERATIONS" \
  -libdir=./bin/lib \
  -shell="$BENCH_SHELL" \
  ${BENCH_COMPARE_BINARIES:+-compare-binaries="$BENCH_COMPARE_BINARIES"} \
  ${SHEET_DESCRIPTION:+-sheet-desc="$SHEET_DESCRIPTION"} \
  ${BENCH_TIMEOUT:+-timeout="$BENCH_TIMEOUT"} \
  ${BENCH_PUBLISH_DIR:+-publishdir="$BENCH_PUBLISH_DIR"} \
  ${BENCH_COMPARE_DIR:+-comparedir="$BENCH_COMPARE_DIR"} \
  ${BENCH_EXCLUDE:+-exclude="$BENCH_EXCLUDE"} \
  -- "$TEST_ARGS"
