#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root

# GCAssert and unused need generated files in the workspace to work properly.
# generated files requirements -- begin
bazel run //pkg/gen:code
bazel run //pkg/cmd/generate-cgo:generate-cgo --run_under="cd $root && "
# generated files requirements -- end

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- \
    test --config=ci --define gotags=bazel,gss,nightly,lint \
    --test_env=CC=$(which gcc) \
    --test_env=CXX=$(which gcc) \
    --test_env=HOME \
    --sandbox_writable_path=$HOME \
    --test_env=GO_SDK=$(dirname $(dirname $(bazel run @go_sdk//:bin/go --run_under=realpath))) \
    --test_env=COCKROACH_WORKSPACE=$root
