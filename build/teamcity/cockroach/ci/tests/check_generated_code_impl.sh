#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root, check_workspace_clean

mkdir -p artifacts

# Buffer noisy output and only print it on failure.
if ! (./build/bazelutil/check.sh &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)); then
    # The command will output instructions on how to fix the error.
    exit 1
fi
rm artifacts/buildshort.log

build/bazelutil/bazel-generate.sh \
  BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e COCKROACH_BAZEL_FORCE_GENERATE=1" \
  &> artifacts/buildshort.log || (cat artifacts/buildshort.log && false)

rm artifacts/buildshort.log
if grep TODO DEPS.bzl; then
    echo "Missing TODO comment in DEPS.bzl. Did you run \`./dev generate bazel --mirror\`?"
    exit 1
fi
check_workspace_clean "Run \`./dev generate bazel\` to automatically regenerate these."

# Run go mod tidy and ensure nothing changes.
# NB: If files are missing from any packages then `go mod tidy` will
# fail. So we need to make sure that `.pb.go` sources are populated.
bazel run //pkg/gen:go_proto
bazel run @go_sdk//:bin/go --ui_event_filters=-DEBUG,-info,-stdout,-stderr --noshow_progress mod tidy
check_workspace_clean "Run \`go mod tidy\` to automatically regenerate these."
