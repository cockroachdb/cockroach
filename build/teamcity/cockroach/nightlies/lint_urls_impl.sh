#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

# GCAssert and unused need generated files in the workspace to work properly.
# generated files requirements -- begin
bazel run //pkg/gen:code
bazel run //pkg/cmd/generate-cgo:generate-cgo --run_under="cd $(bazel info workspace) && "
# generated files requirements -- end

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
exit_status=0
XML_OUTPUT_FILE=/artifacts/test.xml GO_TEST_WRAP_TESTV=1 GO_TEST_WRAP=1 bazel \
    run --config=ci --config=test --define gotags=bazel,gss,nightly,lint \
    //build/bazelutil:lint || exit_status=$?
# The schema of the output test.xml will be slightly wrong -- ask `bazci` to fix
# it up.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci munge-test-xml /artifacts/test.xml
exit $exit_status
