#!/usr/bin/env bash

set -ex

./build/builder.sh env \
    GOOGLE_EPHEMERAL_CREDENTIALS="$GOOGLE_EPHEMERAL_CREDENTIALS" \
    TC_BUILD_ID="$TC_BUILD_ID" \
    ./build/nightly-workload.sh
