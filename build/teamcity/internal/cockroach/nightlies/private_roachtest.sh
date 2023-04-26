#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

export TESTS="${TESTS:-costfuzz/workload-replay}"
export ROACHTEST_BUCKET="${ROACHTEST_BUCKET:-cockroach-nightly-private}"
export GCE_PROJECT="e2e-infra-381422"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e BUILD_VCS_NUMBER -e CLOUD=gce -e TESTS -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e ROACHTEST_BUCKET -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e COCKROACH_DEV_LICENSE -e SFUSER -e SFPASSWORD -e GCE_PROJECT" \
			       run_bazel build/teamcity/internal/cockroach/nightlies/private_roachtest_impl.sh
