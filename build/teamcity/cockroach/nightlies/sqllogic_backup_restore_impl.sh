#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"  # For process_test_json
source "$dir/teamcity-support.sh"

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"

log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

ARTIFACTS_DIR=/artifacts
exit_status=0

configs=(
local
multiregion-9node-3region-3azs
5node
multiregion-9node-3region-3azs-no-los
multiregion-9node-3region-3azs-tenant
multiregion-9node-3region-3azs-vec-off
multiregion-15node-5region-3azs
3node-tenant
3node-tenant-multiregion
)

for config in "${configs[@]}"; do
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
    //pkg/ccl/logictestccl/tests/$config/... \
    --test_arg=-show-sql \
    --test_env=COCKROACH_LOGIC_TEST_BACKUP_RESTORE_PROBABILITY=0.5 \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_timeout=28800 \
    --test_env=GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS" \
    || exit_status=$?

done
