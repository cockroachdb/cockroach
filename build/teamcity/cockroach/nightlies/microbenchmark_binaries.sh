#!/usr/bin/env bash
#
# This script builds portable test binaries required by the microbenchmarks weekly script.
# It is intended to be used on-demand when new binaries are required.
# Parameters:
#   BENCH_PACKAGE: package to build (default: ./pkg/...)
#   GCS_BINARIES_DIR: GCS directory to copy test binaries archive to. (default: gs://cockroach-microbenchmarks/binaries)
#   BINARIES_NAME: name of the test binaries archive (default: <branch/tag>-<commit>.tar.gz)

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud


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

# Build test binaries
./dev test-binaries "$BENCH_PACKAGE" --docker-args="$docker_args"

# Copy binaries to bucket
gsutil cp ./bin/test_binaries.tar.gz "$GCS_BINARIES_DIR/$BINARIES_NAME.tar.gz"
