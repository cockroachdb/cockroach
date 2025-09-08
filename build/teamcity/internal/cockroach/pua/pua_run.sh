#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel
#
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e GOOGLE_APPLICATION_CREDENTIALS_CONTENT -e GOOGLE_SERVICE_ACCOUNT -e GOOGLE_PROJECT -e PUA_CONFIG -e CRDB_VERSION -e CRDB_UPGRADE_VERSION -e DD_API_KEY -e DD_APP_KEY" \
			       run_bazel build/teamcity/internal/cockroach/pua/pua_run_impl.sh
