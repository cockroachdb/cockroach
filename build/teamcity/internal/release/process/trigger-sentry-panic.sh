#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

if [[ -n "${DRY_RUN}" ]] ; then
  echo "Skipping this step in dry-run mode"
  exit
fi

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e VERSION -e GOOGLE_CREDENTIALS -e SENTRY_AUTH_TOKEN -e GITHUB_TOKEN" run_bazel << 'EOF'
bazel build //pkg/cmd/release/sentry
BAZEL_BIN=$(bazel info bazel-bin)
$BAZEL_BIN/pkg/cmd/release/sentry/sentry_/sentry
EOF
