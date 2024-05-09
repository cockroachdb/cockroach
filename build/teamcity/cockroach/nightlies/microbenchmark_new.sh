#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# Set up credentials
google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
generate_ssh_key
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
export ROACHPROD_USER=teamcity
export ROACHPROD_CLUSTER=teamcity-microbench-${TC_BUILD_ID}

## Build rochprod and roachprod-microbench
#run_bazel <<'EOF'
#bazel build --config ci --config crosslinux //pkg/cmd/roachprod //pkg/cmd/roachprod-microbench
#BAZEL_BIN=$(bazel info bazel-bin --config ci --config crosslinux)
#mkdir -p bin
#cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
#cp $BAZEL_BIN/pkg/cmd/roachprod-microbench/roachprod-microbench_/roachprod-microbench bin
#chmod a+w bin/roachprod bin/roachprod-microbench
#EOF

## Create roachprod cluster
#./bin/roachprod create "$ROACHPROD_CLUSTER" -n 1 \
#  --lifetime 2h \
#  --clouds gce
#
#echo "hello"
#
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e BUILD_VCS_NUMBER -e CLOUD -e COCKROACH_DEV_LICENSE -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL" \
			       run_bazel build/teamcity/cockroach/nightlies/microbenchmark_build_tests_impl.sh
#
#echo "world"
