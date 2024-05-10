#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e sha=$BUILD_VCS_NUMBER" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/github-action-poller
BAZEL_BIN=$(bazel info bazel-bin --config ci)
$BAZEL_BIN/pkg/cmd/github-action-poller/github-action-poller_/github-action-poller \
  --owner cockroachdb \
  --repo cockroach \
  --sha $sha \
  --timeout=40m \
  --sleep=30s \
  acceptance \
  check_generated_code \
  docker_image_amd64 \
  examples_orms \
  lint \
  linux_amd64_build \
  linux_amd64_fips_build \
  linux_arm64_build \
  local_roachtest \
  local_roachtest_fips \
  macos_amd64_build \
  macos_arm64_build \
  unit_tests \
  windows_build
EOF
