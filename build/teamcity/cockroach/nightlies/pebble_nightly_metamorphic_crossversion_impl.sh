#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

set -euxo pipefail

ARTIFACTS_DIR=$PWD/artifacts/meta
mkdir -p "${ARTIFACTS_DIR}"
chmod o+rwx "${ARTIFACTS_DIR}"
chmod -R o+rwx "/test-bin"
ls -l "/test-bin"

echo "TC_SERVER_URL is $TC_SERVER_URL"

bazel build //pkg/cmd/bazci

BAZEL_BIN=$(bazel info bazel-bin)

# The script accepts the arguments accepted by TestMetaCrossVersion. It should
# look like:
#
#   --version release-21.2,f390aeb3d,f390aeb3d.test --version release-22.1,c5e43d21,c5e43d21.test
#
# We need to pass these same arguments to the test invocation. To do that,
# prefix each argument with `--test_arg `, so that we can instruct bazel
# to set the arguments appropriately.
test_args=$(echo $@ | python3 -c "import sys; print(' '.join(['--test_arg=' +word.strip() for word in sys.stdin.read().split(' ')]))")

# Add the verbosity and artifacts flags.
test_args="--test_arg=-artifacts --test_arg ${ARTIFACTS_DIR} --test_arg=-test.v $test_args"

$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- \
                                      test @com_github_cockroachdb_pebble//internal/metamorphic/crossversion:crossversion_test \
                                      --test_env TC_SERVER_URL=$TC_SERVER_URL \
                                      --test_timeout=25200 '--test_filter=TestMetaCrossVersion$' \
                                      --define gotags=bazel,invariants \
                                      --run_under "@com_github_cockroachdb_stress//:stress -bazel -shardable-artifacts 'XML_OUTPUT_FILE=$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci merge-test-xmls' -maxtime 6h -maxfails 1 -timeout 60m -stderr -p 1" \
                                      $test_args \
                                      --test_output streamed
