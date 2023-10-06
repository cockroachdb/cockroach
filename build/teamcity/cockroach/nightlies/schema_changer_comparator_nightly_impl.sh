#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

CORPUS_DIR=/artifacts/logictest-stmts-corpus-dir # dir to store all collected corpus file(s)
exit_status=0

# Collect sql logic tests statements corpus.
bazel run -- //pkg/cmd/generate-logictest-corpus:generate-logictest-corpus \
-out-dir=$CORPUS_DIR  \
|| exit_status=$?

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

# Run schema changer comparator test with statements from the collected corpus file(s).
for CORPUS_FILE in "$CORPUS_DIR"/*
do
  $BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
    //pkg/sql/schemachanger:schemachanger_test \
    --test_arg=--logictest-stmt-corpus-path="$CORPUS_FILE" \
    --test_filter='^TestComparatorFromLogicTests$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_timeout=7200 \
    || exit_status=$?
done

exit $exit_status
