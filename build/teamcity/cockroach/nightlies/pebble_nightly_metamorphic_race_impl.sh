#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

set -euxo pipefail
ARTIFACTS_DIR=/artifacts/meta
mkdir -p $ARTIFACTS_DIR

echo "TC_SERVER_URL is $TC_SERVER_URL"

bazel build //pkg/cmd/bazci

BAZEL_BIN=$(bazel info bazel-bin)

exit_status=0
# NB: If adjusting the metamorphic test flags below, be sure to also update
# pkg/cmd/github-post/main.go to ensure the GitHub issue poster includes the
# correct flags in the reproduction command.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --formatter=pebble-metamorphic -- test --config=race --config=ci \
                                      @com_github_cockroachdb_pebble//internal/metamorphic:metamorphic_test \
                                      --test_env TC_SERVER_URL=$TC_SERVER_URL \
                                      --test_timeout=14400 \
                                      --test_sharding_strategy=disabled \
                                      --define gotags=bazel,invariants \
                                      --run_under "@com_github_cockroachdb_stress//:stress -bazel -shardable-artifacts 'XML_OUTPUT_FILE=$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci merge-test-xmls' -maxtime 3h -maxfails 1 -timeout 60m -stderr -p 1" \
                                      --test_arg -dir --test_arg $ARTIFACTS_DIR \
                                      --test_arg -ops --test_arg "uniform:2000-5000" \
                                      --test_output streamed \
    || exit_status=$?

exit $exit_status
