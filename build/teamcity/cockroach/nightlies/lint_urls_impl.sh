#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root

# GCAssert and unused need generated files in the workspace to work properly.
# generated files requirements -- begin
bazel run //pkg/gen:code
bazel run //pkg/cmd/generate-cgo:generate-cgo --run_under="cd $root && "
# generated files requirements -- end

bazel build //pkg/cmd/bazci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci -- \
    test //pkg/testutils/lint:lint_test \
    --config=ci --define gotags=bazel,gss,nightly,lint \
    --test_filter=TestNightlyLint \
    --test_env=CC=$(which gcc) \
    --test_env=CXX=$(which gcc) \
    --test_env=HOME \
    --sandbox_writable_path=$HOME \
    --test_env=GO_SDK=$(dirname $(dirname $(bazel run @go_sdk//:bin/go --run_under=realpath))) \
    --test_env=COCKROACH_WORKSPACE=$root
