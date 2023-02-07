#!/usr/bin/env bash

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
"build --config=crosslinux"
"test --test_tmpdir=/tmp/cockroach"
)
printf "%s\n" "${bazelOpts[@]}" > ~/.bazelrc
./dev doctor

# Build required tooling
./dev build roachprod
./dev build roachprod-microbench
./dev build libgeos

# Copy comparison binaries to bin directory if specified
if [[ -n "${GCS_COMPARE_BINARIES}" ]] ; then
  gsutil cp "$GCS_COMPARE_BINARIES" "$BENCH_COMPARE_BINARIES"
fi

# Build test binaries
./dev test-binaries "$BENCH_PACKAGE"

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
  -libdir=./lib \
  -shell="$BENCH_SHELL" \
  ${BENCH_COMPARE_BINARIES:+-compare-binaries="$BENCH_COMPARE_BINARIES"} \
  ${SHEET_DESCRIPTION:+-sheet-desc="$SHEET_DESCRIPTION"} \
  ${BENCH_TIMEOUT:+-timeout="$BENCH_TIMEOUT"} \
  ${BENCH_PUBLISH_DIR:+-publishdir="$BENCH_PUBLISH_DIR"} \
  ${BENCH_COMPARE_DIR:+-comparedir="$BENCH_COMPARE_DIR"} \
  ${BENCH_EXCLUDE:+-exclude="$BENCH_EXCLUDE"} \
  -- "$TEST_ARGS"
