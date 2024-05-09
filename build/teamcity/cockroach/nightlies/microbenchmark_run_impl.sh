#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
generate_ssh_key
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
export ROACHPROD_USER=teamcity
export ROACHPROD_CLUSTER=teamcity-microbench-${TC_BUILD_ID}

mkdir -p "$PWD/bin"
chmod o+rwx "$PWD/bin"

# Build the roachprod binary.
bazel build //pkg/cmd/roachprod --config ci -c opt
BAZEL_BIN=$(bazel info bazel-bin --config ci -c opt)
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
chmod a+w bin/roachprod

# Build the roachprod-microbench binary.
bazel build //pkg/cmd/roachprod-microbench --config ci -c opt
BAZEL_BIN=$(bazel info bazel-bin --config ci -c opt)
cp $BAZEL_BIN/pkg/cmd/roachprod-microbench/roachprod-microbench_/roachprod-microbench bin
chmod a+w bin/roachprod-microbench

./bin/roachprod --help
./bin/roachprod-microbench --help

# Create roachprod cluster
./bin/roachprod create "$ROACHPROD_CLUSTER" -n 1 \
  --lifetime 1h \
  --clouds gce
