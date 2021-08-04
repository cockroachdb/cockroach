#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
XML_OUTPUT_FILE=/artifacts/test.xml GO_TEST_WRAP_TESTV=1 GO_TEST_WRAP=1 bazel \
	       run --config=ci --config=test //build/bazelutil:lint
# The schema of the output test.xml will be slightly wrong -- ask `bazci` to fix
# it up.
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci munge-test-xml /artifacts/test.xml
