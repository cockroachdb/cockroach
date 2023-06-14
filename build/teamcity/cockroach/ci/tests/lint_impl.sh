#!/usr/bin/env bash

set -xeuo pipefail

# GCAssert requirements -- start
bazel run //pkg/gen:code
bazel run //pkg/cmd/generate-cgo:generate-cgo --run_under="cd $(bazel info workspace) && "
# GCAssert requirements -- end

bazel build //pkg/cmd/bazci --config=ci
XML_OUTPUT_FILE=/artifacts/test.xml GO_TEST_WRAP_TESTV=1 GO_TEST_WRAP=1 CC=$(which gcc) CXX=$(which gcc) bazel \
	       run --config=ci --config=test //build/bazelutil:lint
# The schema of the output test.xml will be slightly wrong -- ask `bazci` to fix
# it up.
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci munge-test-xml /artifacts/test.xml
