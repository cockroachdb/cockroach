#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root, check_workspace_clean

mkdir -p artifacts

begin_check_generated_code_tests

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
check_workspace_clean 'dev_generate_bazel' "Run \`./dev generate bazel\` to automatically regenerate these."

# Run `bazel run //pkg/gen` and ensure nothing changes. This ensures
# generated documentation and checked-in go code are up to date.
bazel run //pkg/gen
check_workspace_clean 'dev_generate' "Run \`./dev generate\` to automatically regenerate these."
# Run go mod tidy and ensure nothing changes.
# NB: If files are missing from any packages then `go mod tidy` will
# fail. So we need to make sure that `.pb.go` sources are populated.
# This is part of what //pkg/gen does, in addition to generating Go code and
# docs.
bazel run @go_sdk//:bin/go --ui_event_filters=-DEBUG,-info,-stdout,-stderr --noshow_progress mod tidy
check_workspace_clean 'go_mod_tidy' "Run \`go mod tidy\` to automatically regenerate these."
# Run `generate-logictest` and ensure nothing changes.
bazel run //pkg/cmd/generate-logictest -- -out-dir="$PWD"
check_workspace_clean 'generate_logictest' "Run \`./dev gen testlogic\` to automatically regenerate these."

# NB: If this step fails, then some checksum in the code is probably not
# matching up to the "real" checksum for that artifact.
bazel fetch @distdir//:archives

end_check_generated_code_tests
