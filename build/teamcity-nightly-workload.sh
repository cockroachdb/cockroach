#!/usr/bin/env bash

set -ex

mkdir -p artifacts

# Build the cockroach binary (which uses docker) before hopping into docker for
# the nightly-workload.sh script.
pkg/acceptance/prepare.sh

# Use this instead of prepare.sh for faster debugging iteration.
# curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach-linux-2.6.32-gnu-amd64
# chmod +x cockroach-linux-2.6.32-gnu-amd64

./build/builder.sh env \
    AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    GOOGLE_CREDENTIALS="$GOOGLE_CREDENTIALS" \
    TC_BUILD_ID="$TC_BUILD_ID" \
    ./build/nightly-workload.sh
