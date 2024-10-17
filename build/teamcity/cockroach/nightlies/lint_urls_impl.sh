#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
exit_status=0
XML_OUTPUT_FILE=/artifacts/test.xml GO_TEST_WRAP_TESTV=1 GO_TEST_WRAP=1 bazel \
    run --config=ci --config=test --define gotags=bazel,gss,nightly \
    //build/bazelutil:lint || exit_status=$?
# The schema of the output test.xml will be slightly wrong -- ask `bazci` to fix
# it up.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci munge-test-xml /artifacts/test.xml
