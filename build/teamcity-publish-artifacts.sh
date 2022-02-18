#!/usr/bin/env bash

# This script is called by the build configuration "Publish Bleeding Edge" in TeamCity.

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e TC_BUILD_BRANCH" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
$BAZEL_BIN/pkg/cmd/publish-artifacts/publish-artifacts_/publish-artifacts
EOF
