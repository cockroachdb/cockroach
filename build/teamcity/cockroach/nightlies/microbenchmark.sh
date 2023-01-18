#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
export ROACHPROD_USER=teamcity

# Simulate env vars
export CLUSTER_LIFETIME=24h
export GCE_NODE_COUNT=8
export GCE_MACHINE_TYPE=n2d-standard-8
export GCE_ZONE=us-central1-b

export BENCH_PACKAGE=./pkg/...
export BENCH_ITERATIONS=1
export TEST_ARGS="-test.benchtime=1x"

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

# Build test binaries
./dev test-binaries "$BENCH_PACKAGE"

# Create roachprod cluster
./bin/roachprod create teamcity-microbench -n "$GCE_NODE_COUNT" \
  --lifetime "$CLUSTER_LIFETIME" \
  --clouds gce \
  --gce-machine-type "$GCE_MACHINE_TYPE" \
  --gce-zones="$GCE_ZONE" \
  --os-volume-size=256

# Execute microbenchmarks
./bin/roachprod-microbench ./artifacts/microbench \
  -cluster teamcity-microbench \
  -iterations $BENCH_ITERATIONS \
  -libdir=./lib \
  ${BENCH_PUBLISH_DIR:+-publishdir="$BENCH_PUBLISH_DIR"} \
  ${BENCH_COMPARE_DIR:+-comparedir="$BENCH_COMPARE_DIR"} \
  -- "$TEST_ARGS"

#  ${BENCH_EXCLUDE:+-exclude="$BENCH_EXCLUDE"} \
