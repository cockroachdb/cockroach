#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

export TESTS="${TESTS:-costfuzz/workload-replay}"
export ROACHTEST_BUCKET="${ROACHTEST_BUCKET:-cockroach-nightly-private}"
export GCE_PROJECT="e2e-infra-381422"
export BACKUP_TESTING_BUCKET="cockroach-backup-testing-private"

# TODO(msbutler): use a different bucket once it is created. Sadly, I don't have the permissions
#  currently to create a new bucket in this gce project.
export BACKUP_TESTING_BUCKET_LONG_TTL="cockroach-backup-testing-private"
export COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=1
export COCKROACH_NO_EXAMPLE_DATABASE=1
export COCKROACH_AUTO_BALLAST=false

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e BUILD_VCS_NUMBER -e CLOUD=gce -e TESTS -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e ROACHTEST_PRIVATE -e ROACHTEST_BUCKET -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e COCKROACH_DEV_LICENSE -e BACKUP_TESTING_BUCKET -e BACKUP_TESTING_BUCKET_LONG_TTL -e SFUSER -e SFPASSWORD -e COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING -e COCKROACH_NO_EXAMPLE_DATABASE -e  COCKROACH_AUTO_BALLAST -e GCE_PROJECT" \
			       run_bazel build/teamcity/internal/cockroach/nightlies/private_roachtest_impl.sh
