#!/usr/bin/env bash
#
# This script builds portable test binaries required by the microbenchmarks weekly script.
# It is intended to be used on-demand when new binaries are required.
# Parameters:
#   BENCH_PACKAGE: package to build (default: ./pkg/...)
#   GCS_BINARIES_DIR: GCS directory to copy test binaries archive to. (default: gs://cockroach-microbenchmarks/binaries)

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

# Build test binaries
./dev test-binaries "$BENCH_PACKAGE"
bin_name=$(git symbolic-ref --short HEAD)-$(git rev-parse --short HEAD).tar.gz

# Copy binaries to bucket
gsutil cp ./bin/test-binaries.tar.gz "$GCS_BINARIES_DIR/$bin_name.tar.gz"
