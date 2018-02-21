#!/usr/bin/env bash

set -e

./build/builder.sh env \
    GOOGLE_EPHEMERAL_CREDENTIALS="$GOOGLE_EPHEMERAL_CREDENTIALS" \
    TC_BUILD_ID="$TC_BUILD_ID" \
    COCKROACH_DEV_LICENSE="$COCKROACH_DEV_LICENSE" \
    ./build/roachtest-nightly.sh
