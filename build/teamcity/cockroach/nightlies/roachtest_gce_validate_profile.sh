#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# Run only these tests. This is hard-coded intentionally so that the set of
# tests to invoke is tracked through the main git repository.
export TESTS="${TESTS:-sysbench/oltp_read_write/nodes=3/cpu=8/conc=64}"
export COUNT="${COUNT:-20}"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e BUILD_VCS_NUMBER -e CLOUD -e COCKROACH_DEV_LICENSE -e TESTS -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e GOOGLE_KMS_KEY_A -e GOOGLE_KMS_KEY_B -e GOOGLE_CREDENTIALS_ASSUME_ROLE -e GOOGLE_SERVICE_ACCOUNT -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e SELECT_PROBABILITY=1.0 -e COCKROACH_RANDOM_SEED -e ROACHTEST_ASSERTIONS_ENABLED_SEED -e ROACHTEST_FORCE_RUN_INVALID_RELEASE_BRANCH -e GRAFANA_SERVICE_ACCOUNT_JSON -e GRAFANA_SERVICE_ACCOUNT_AUDIENCE -e ARM_PROBABILITY=0.0 -e USE_SPOT -e SELECTIVE_TESTS -e SFUSER -e SFPASSWORD -e COCKROACH_EA_PROBABILITY=0.0 -e EXPORT_OPENMETRICS -e ROACHPERF_OPENMETRICS_CREDENTIALS" \
			       run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_impl.sh

benchstatfile="${root}/artifacts/benchstat.txt"
find "${root}/artifacts" -type f -name bench.txt -exec cat {} ';' | tee "$benchstatfile"
# Delete file if empty.
[ -s "${benchstatfile}" ] || rm -f "${benchstatfile}"
