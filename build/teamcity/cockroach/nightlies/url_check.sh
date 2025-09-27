#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run url check"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GITHUB_API_TOKEN -e GOOGLE_EPHEMERAL_CREDENTIALS -e TC_BUILD_BRANCH" run_bazel build/teamcity/cockroach/nightlies/url_check_impl.sh
tc_end_block "Run url check"
