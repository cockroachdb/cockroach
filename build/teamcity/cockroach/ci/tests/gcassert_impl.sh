#!/usr/bin/env bash

set -xeuo pipefail

bazel build @com_github_jordanlewis_gcassert//cmd/gcassert:gcassert --config=ci
bazel run //pkg/gen:code
WORKSPACE=$(bazel info workspace --color=no)
BAZEL_BIN=$(bazel info bazel-bin --color=no)
bazel run //pkg/gen:code
bazel run //pkg/cmd/generate-cgo:generate-cgo -- --workspace $WORKSPACE --bazel-bin $BAZEL_BIN
GODIR=$(dirname $(bazel run @go_sdk//:bin/go --run_under=realpath))
echo "##teamcity[testStarted name='GcAssert' captureStandardOutput='true']"
exit_status=0
PATH=$GODIR:$PATH $(bazel info bazel-bin --config=ci)/external/com_github_jordanlewis_gcassert/cmd/gcassert/gcassert_/gcassert $(cat ./pkg/testutils/lint/gcassert_paths.txt | sed 's|^|./pkg/|') || exit_status=$?
if [ "$exit_status" -ne 0 ]; then
    echo "##teamcity[testFailed name='GcAssert']"
fi
echo "##teamcity[testFinished name='GcAssert']"
cp /tmp/gcassert* $PWD/artifacts

